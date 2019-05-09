# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for FSL FEAT results

This uses the NIDM results implementation for FSL.

"""


from datalad_metalad.extractors.base import MetadataExtractor
from datalad_metalad import (
    get_file_id,
)
from six import (
    text_type,
)
from datalad.support.json_py import (
    load as jsonload,
)
from datalad.utils import (
    Path,
    make_tempfile,
)

import logging
lgr = logging.getLogger('datalad.metadata.extractors.fslfeat')


class FSLFEATExtractor(MetadataExtractor):
    def __call__(self, dataset, refcommit, process_type, status):
        # shortcut
        ds = dataset

        feat_dirs = []

        for s in status:
            path = Path(s['path'])
            if path.name == 'design.fsf' and (path.parent / 'stats').exists():
                feat_dirs.append(path.parent)

        if not feat_dirs:
            return

        context = None
        extracts = []
        for fd in feat_dirs:
            # TODO protect against failure and yield error result
            res = _extract_nidmfsl(fd)
            if '@context' not in res or '@graph' not in res:
                # this is an unexpected output, fail, we cannot work with it
                # TODO error properly
                raise ValueError('not an expected report')
                # TODO can the context possibly vary across reports?
            context = res['@context']
            extracts.append(res['@graph'])

        yield dict(
            metadata={
                '@context': context,
                '@graph': extracts,
            },
            type='dataset',
            status='ok',
        )


# TODO which files are really needed for nidmfsl (can we skip, e.g. res4d.nii.gz)?


def _extract_nidmfsl(feat_dir):
    from nidmfsl.fsl_exporter.fsl_exporter import FSLtoNIDMExporter
    with make_tempfile(mkdir=True) as tmpdir:
        exporter = FSLtoNIDMExporter(
            out_dirname=tmpdir,
            zipped=False,
            feat_dir=text_type(feat_dir),
            # this is all fake, we cannot know it, but NIDM FSL wants it
            # TODO try fishing it out from the result again
            groups=[['control', 1]])
        exporter.parse()
        outdir = exporter.export()
        md = jsonload(text_type(Path(outdir) / 'nidm.json'))
    return md
